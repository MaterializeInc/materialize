# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Session rebinding: names resolve per session, and cursors keep a snapshot.

A durable view `v` is replaced over and over between two fixed input tables.
Sessions of this component's own pool shadow it with temporary views, run
prepared statements with drawn parameters, and page through cursors.
`session_rebinding_model` gives the rows a binding yields under the
parameters, and how a cursor pages through them.

Every definition of `v`, durable or temporary, selects a marker of its own,
so a result says which definition produced it. A result must be exactly the
projection of the input its marker names, at one timestamp. A session that
has a temporary view must read that one through the unqualified name and
never another session's, and a session without one must read the durable
view. A cursor must return every page from the definition and timestamp of
its first page, whatever replaces the view meanwhile, and `COMMIT` must
destroy it. Separately, a read must not return a durable definition that an
acknowledged replacement superseded before the read began. The assertion
messages are those of the legacy session rebinding driver, except that last
one.

Temporary views and prepared statements die with their connection, so their
state is kept per connection object and is known exactly. A temporary view
is uncertain only while a statement that changes it has no definite answer
on a connection that survived, and then a read accepts every candidate.

An unqualified name in a statement prepared before a temporary view was
created need not resolve to it, so unqualified reads plan afresh.

NOTE: the assertion on superseded definitions relies on a read seeing DDL
acknowledged before it began. A fenced environmentd that still answers a
read from its own catalog after its successor acknowledged the replacement
would fail it.
"""

from __future__ import annotations

import threading
import time
import weakref
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis import session_rebinding_model as model
from materialize.antithesis_driver.client import Op, Outcome, Pool, Status, Target
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "rebind"
SESSIONS = 3
INPUTS: tuple[tuple[model.Row, ...], tuple[model.Row, ...]] = (
    (
        (1, 10, "t0"),
        (2, None, "t0"),
        (3, -30, "t0"),
        (4, 40, "t0"),
        (5, 50, "t0"),
        (6, 0, "t0"),
    ),
    ((1, 11, "t1"), (2, 21, "t1"), (3, None, "t1"), (7, -71, "t1"), (8, 81, "t1")),
)
SETTINGS = {
    "cluster": "quickstart",
    "transaction_isolation": "'strict serializable'",
    "search_path": SCHEMA,
}
LINGER_SECONDS = (0.0, 0.05, 0.3, 1.0, 3.0)
"""How long a cursor waits between its first page and the rest, so that
replacements land in between."""
MAX_PAGES = 16
"""More pages than any cursor over the inputs has."""

Result = tuple[Any, ...]
"""`id, projected, generation, marker, read timestamp`."""
Pages = list[tuple[int | None, list[Result]]]
"""Each `FETCH` of a cursor: its count, None for `ALL`, and its rows."""


def definition(marker: int, binding: int) -> str:
    return f"SELECT id, v, generation, {marker}::bigint AS gen FROM {SCHEMA}.t{binding}"


def query(source: str, descending: bool) -> str:
    return (
        "SELECT id, v * $1::bigint + $2::bigint AS projected, generation, gen,"
        f" mz_now()::text AS read_ts FROM {source}"
        f" ORDER BY id {'DESC' if descending else 'ASC'}"
    )


def literal_query(source: str, parameters: model.Parameters) -> str:
    return (
        query(source, parameters.descending)
        .replace("$1", str(parameters.factor))
        .replace("$2", str(parameters.offset))
    )


def run(conn: psycopg.Connection, statement: str) -> psycopg.Cursor[Result]:
    # psycopg prepares a statement it has run a few times. A plan it keeps
    # for an unqualified name would hide a temporary view created later.
    return conn.execute(statement.encode(), prepare=False)


def setup_statements() -> list[str]:
    statements = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"]
    for binding, rows in enumerate(INPUTS):
        table = f"{SCHEMA}.t{binding}"
        values = ", ".join(
            f"({key}, {'NULL' if value is None else value}::bigint, '{tag}')"
            for key, value, tag in rows
        )
        statements += [
            f"CREATE TABLE IF NOT EXISTS {table} (id bigint, v bigint, generation text)",
            # Setup can run again after a lost response.
            f"INSERT INTO {table} SELECT * FROM (VALUES {values}) AS rows"
            f" WHERE NOT EXISTS (SELECT 1 FROM {table})",
        ]
    statements.append(f"CREATE OR REPLACE VIEW {SCHEMA}.v AS {definition(0, 0)}")
    return statements


@dataclass
class Replace:
    """One `CREATE OR REPLACE VIEW` of the durable view."""

    marker: int
    binding: int
    start: float
    """No later than the server received it."""
    end: float | None = None
    outcome: str = "pending"
    """`pending`, `ok`, `rejected` or `unknown`."""


def allowed(replaces: Sequence[Replace], start: float, end: float) -> dict[int, int]:
    """The durable definitions a read over `[start, end]` may return, each
    with its binding.

    A replacement without a definite answer may take effect at any later
    time. An acknowledged one is ruled out once another acknowledged
    replacement began after it ended and ended before the read began.
    """
    acked = [r for r in replaces if r.outcome == "ok" and r.end is not None]
    result = {}
    for r in replaces:
        if r.outcome == "rejected" or r.start > end:
            continue
        superseded = (
            r.outcome == "ok"
            and r.end is not None
            and any(
                o.start > r.end and o.end is not None and o.end < start for o in acked
            )
        )
        if not superseded:
            result[r.marker] = r.binding
    return result


def verdict(
    rows: Sequence[Result], parameters: model.Parameters, definitions: dict[int, int]
) -> tuple[int | None, str | None]:
    """The marker of the one definition `rows` came from, or what is wrong.

    `definitions` maps the marker of every definition ever attempted to its
    binding. Both inputs are nonempty, so a result has rows.
    """
    if not rows or any(len(row) != 5 for row in rows):
        return None, "no rows or malformed rows"
    markers = {row[3] for row in rows}
    timestamps = {str(row[4]) for row in rows}
    if len(markers) != 1:
        return None, "rows of several definitions"
    marker = next(iter(markers))
    if len(timestamps) != 1 or not all(t.isdigit() for t in timestamps):
        return marker, "rows of several timestamps"
    if marker not in definitions:
        return marker, "a marker that no definition has"
    expected = parameters.project(INPUTS[definitions[marker]])
    if tuple(tuple(row[:3]) for row in rows) != expected:
        return marker, "rows differ from the projection of the definition's input"
    return marker, None


def paged(
    pages: Sequence[tuple[int | None, Sequence[Result]]],
    parameters: model.Parameters,
    binding: int,
) -> bool:
    """Whether a cursor's `FETCH count` pages are those of the model's cursor."""
    cursor = model.Cursor(parameters.project(INPUTS[binding]))
    return all(
        tuple(tuple(row[:3]) for row in page) == cursor.fetch(count)
        for count, page in pages
    )


@dataclass
class ConnState:
    """What one connection holds. Only its session's thread touches it."""

    prepared: dict[bool, tuple[str, float]] = field(default_factory=dict)
    """By sort direction, the qualified prepared statement and when it was
    prepared."""
    temp: int | None = None
    """The marker of the temporary view, if the connection has one."""
    temp_since: float = 0.0
    uncertain: bool = False
    """A statement that changes the temporary view has no definite answer."""
    candidates: set[int] = field(default_factory=set)
    """While uncertain, the markers of the temporary views that may exist."""


@dataclass(frozen=True)
class Shadow:
    """A connection's temporary view as an unqualified read found it."""

    temp: int | None
    since: float
    uncertain: bool
    candidates: frozenset[int]


class Rebinding:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.pool = Pool("rebind", Target(ctx.config.host), SESSIONS)
        self._lock = threading.Lock()
        self._states: weakref.WeakKeyDictionary[psycopg.Connection, ConnState] = (
            weakref.WeakKeyDictionary()
        )
        self.replaces = [Replace(0, 0, float("-inf"), float("-inf"), "ok")]
        self.definitions = {0: 0}
        """Every marker ever attempted, durable ones positive and temporary
        ones negative, with its binding."""
        self._next = 1
        self._reads: dict[int, float] = {}
        """Reads in flight, each with a time no later than its start."""

    def _session(self, d: Draw) -> int:
        return d.integer(0, SESSIONS - 1)

    def _state(self, conn: psycopg.Connection) -> ConnState:
        with self._lock:
            return self._states.setdefault(conn, ConnState())

    def _id(self) -> int:
        with self._lock:
            self._next += 1
            return self._next - 1

    def _parameters(self, d: Draw) -> model.Parameters:
        return model.Parameters(d.integer(-3, 3), d.integer(-100, 100), d.boolean())

    def replace(self, d: Draw) -> Status:
        binding = d.integer(0, 1)
        marker = self._id()
        record = Replace(marker, binding, time.monotonic())
        with self._lock:
            self.definitions[marker] = binding
            self.replaces.append(record)
        statement = (
            f"CREATE OR REPLACE VIEW {SCHEMA}.v AS {definition(marker, binding)}"
        )

        def execute(conn: psycopg.Connection) -> None:
            run(conn, statement)

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                record.start = max(record.start, outcome.start)
                record.end = outcome.end
                record.outcome = (
                    "ok"
                    if outcome.ok
                    else "rejected" if outcome.rejected else "unknown"
                )
                self._prune()

        return self.pool.call(
            self._session(d),
            Op("rebinding replace", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _prune(self) -> None:
        """Forget replacements that no read in flight or to come may return."""
        oldest = min(self._reads.values(), default=time.monotonic())
        live = allowed(self.replaces, oldest, float("inf"))
        self.replaces = [r for r in self.replaces if r.marker in live]

    def _begin_read(self) -> int:
        with self._lock:
            token = self._next
            self._next += 1
            self._reads[token] = time.monotonic()
        return token

    def _end_read(self, token: int) -> None:
        with self._lock:
            del self._reads[token]

    def _judge(
        self,
        path: str,
        rows: Sequence[Result],
        parameters: model.Parameters,
        start: float,
        end: float,
        shadow: Shadow | None,
        pages: Sequence[tuple[int | None, Sequence[Result]]] | None,
        details: dict[str, Any],
    ) -> int | None:
        """Assert one result. Returns its marker if it passed.

        `shadow` is the connection's temporary view for an unqualified read,
        and None for a read through the qualified name.
        """
        with self._lock:
            marker, error = verdict(rows, parameters, self.definitions)
            binding = self.definitions.get(marker) if marker is not None else None
            durable = allowed(self.replaces, start, end)
        if error is None and marker is not None and binding is not None:
            if pages is not None and not paged(pages, parameters, binding):
                error = "pages differ from those of the cursor's snapshot"
            elif shadow is None:
                if marker < 0:
                    error = "a temporary view through the qualified name"
            elif shadow.uncertain:
                if marker < 0 and -marker not in shadow.candidates:
                    error = "a temporary view the session cannot have"
            elif shadow.temp is None:
                if marker < 0:
                    error = "a temporary view the session does not have"
            elif marker != -shadow.temp:
                error = "not the session's own temporary view"
        details = {
            **details,
            "path": path,
            "factor": parameters.factor,
            "offset": parameters.offset,
            "descending": parameters.descending,
            "marker": marker,
        }
        always(
            error is None,
            "session rebinding: results match generated session bindings and begun snapshots",
            {**details, "error": error, "rows": [list(r) for r in rows][:12]},
        )
        if error is not None or marker is None:
            return None
        if marker >= 0:
            always(
                marker in durable,
                "session rebinding: a read never returns a durable view definition that was replaced before it began",
                {**details, "allowed": sorted(durable)},
            )
            if marker not in durable:
                return None
        return marker

    def _replaced_within(self, after: float, before: float) -> bool:
        """Whether a replacement ran wholly inside `(after, before)` and was
        acknowledged."""
        with self._lock:
            return any(
                r.outcome == "ok"
                and r.end is not None
                and after < r.start
                and r.end < before
                for r in self.replaces
            )

    def temp(self, d: Draw) -> Status:
        """Create a session's temporary view over a drawn input, or drop it."""
        drop = d.boolean(0.4)
        binding = d.integer(0, 1)
        marker = self._id()
        if not drop:
            with self._lock:
                self.definitions[-marker] = binding
        create = f"CREATE TEMPORARY VIEW v AS {definition(-marker, binding)}"

        def execute(conn: psycopg.Connection) -> None:
            state = self._state(conn)
            if state.temp is not None:
                state.candidates.add(state.temp)
            state.uncertain = True
            run(conn, "DROP VIEW IF EXISTS mz_temp.v")
            if drop:
                state.temp = None
            else:
                state.candidates.add(marker)
                run(conn, create)
                state.temp = marker
            state.temp_since = time.monotonic()
            state.uncertain = False
            state.candidates = set()

        return self.pool.call(
            self._session(d),
            Op("rebinding temporary view", execute, lambda outcome: None, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _prepared_op(self, parameters: model.Parameters, done: list[bool]) -> Op:
        name = f"rebind_p{self._id()}"
        source = f"{SCHEMA}.v"
        token = self._begin_read()

        def execute(conn: psycopg.Connection) -> tuple[list[Result], float]:
            state = self._state(conn)
            entry = state.prepared.get(parameters.descending)
            if entry is None:
                run(conn, f"PREPARE {name} AS {query(source, parameters.descending)}")
                entry = (name, time.monotonic())
                state.prepared[parameters.descending] = entry
            call = f"EXECUTE {entry[0]}({parameters.factor}, {parameters.offset})"
            return run(conn, call).fetchall(), entry[1]

        def complete(outcome: Outcome[tuple[list[Result], float]]) -> None:
            try:
                if not outcome.ok:
                    return
                assert outcome.value is not None
                rows, prepared_at = outcome.value
                marker = self._judge(
                    "prepared",
                    rows,
                    parameters,
                    outcome.start,
                    outcome.end,
                    None,
                    None,
                    {},
                )
                if marker is None:
                    return
                with self._lock:
                    record = next(
                        (r for r in self.replaces if r.marker == marker), None
                    )
                sometimes(
                    record is not None and record.start > prepared_at,
                    "session rebinding: prepared execution revalidates after durable catalog changes",
                    {"marker": marker},
                )
                done.append(True)
            finally:
                self._end_read(token)

        return Op("rebinding prepared read", execute, complete, SETTINGS)

    def prepared(self, d: Draw) -> Status:
        """Execute a statement over the qualified name, prepared once per
        connection."""
        op = self._prepared_op(self._parameters(d), [])
        return self.pool.call(self._session(d), op, RULE_WAIT_SECONDS)

    def read(self, d: Draw) -> Status:
        """Read through the unqualified name, planned afresh."""
        parameters = self._parameters(d)
        fresh = d.boolean()
        name = f"rebind_f{self._id()}"
        token = self._begin_read()

        def execute(conn: psycopg.Connection) -> tuple[list[Result], Shadow]:
            state = self._state(conn)
            shadow = Shadow(
                state.temp,
                state.temp_since,
                state.uncertain,
                frozenset(state.candidates),
            )
            if not fresh:
                return run(conn, literal_query("v", parameters)).fetchall(), shadow
            run(conn, f"PREPARE {name} AS {query('v', parameters.descending)}")
            call = f"EXECUTE {name}({parameters.factor}, {parameters.offset})"
            try:
                rows = run(conn, call).fetchall()
            except psycopg.Error:
                # A read that lost a race with a replacement fails here. Its
                # statement must not pile up on a connection that survives.
                try:
                    run(conn, f"DEALLOCATE {name}")
                except psycopg.Error:
                    pass
                raise
            run(conn, f"DEALLOCATE {name}")
            return rows, shadow

        def complete(outcome: Outcome[tuple[list[Result], Shadow]]) -> None:
            try:
                if not outcome.ok:
                    return
                assert outcome.value is not None
                rows, shadow = outcome.value
                marker = self._judge(
                    "prepared afresh" if fresh else "unqualified",
                    rows,
                    parameters,
                    outcome.start,
                    outcome.end,
                    shadow,
                    None,
                    {"temp": shadow.temp, "uncertain": shadow.uncertain},
                )
                shadowed = shadow.temp is not None and not shadow.uncertain
                if marker is not None and shadowed:
                    sometimes(
                        self._replaced_within(shadow.since, outcome.start),
                        "session rebinding: temporary shadows remain session local across durable replacement",
                        {"marker": marker},
                    )
            finally:
                self._end_read(token)

        return self.pool.call(
            self._session(d),
            Op("rebinding unqualified read", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def cursor(self, d: Draw) -> Status:
        """Page through a cursor over the qualified name, slowly."""
        parameters = self._parameters(d)
        first, page = d.integer(1, 3), d.integer(1, 3)
        linger = d.choice(LINGER_SECONDS)
        declare = f"DECLARE c CURSOR FOR {literal_query(f'{SCHEMA}.v', parameters)}"
        token = self._begin_read()

        def execute(conn: psycopg.Connection) -> tuple[Pages, float, float, bool]:
            run(conn, "BEGIN")
            run(conn, declare)
            pages: Pages = [(first, run(conn, f"FETCH {first} c").fetchall())]
            first_at = time.monotonic()
            time.sleep(linger)
            last_at = time.monotonic()
            while pages[-1][1] and len(pages) < MAX_PAGES:
                pages.append((page, run(conn, f"FETCH {page} c").fetchall()))
            pages.append((None, run(conn, "FETCH ALL c").fetchall()))
            run(conn, "COMMIT")
            try:
                run(conn, "FETCH ALL c")
                destroyed = False
            except psycopg.errors.InvalidCursorName:
                destroyed = True
            return pages, first_at, last_at, destroyed

        def complete(outcome: Outcome[tuple[Pages, float, float, bool]]) -> None:
            try:
                if not outcome.ok:
                    return
                assert outcome.value is not None
                pages, first_at, last_at, destroyed = outcome.value
                rows = [row for _, rows in pages for row in rows]
                details = {"first": first, "page": page, "linger": linger}
                # The cursor reads at its first page, so only replacements up
                # to then decide which definitions it may return.
                marker = self._judge(
                    "portal",
                    rows,
                    parameters,
                    outcome.start,
                    first_at,
                    None,
                    pages,
                    details,
                )
                always(
                    destroyed,
                    "session rebinding: COMMIT destroys its portals",
                    details,
                )
                if marker is not None:
                    sometimes(
                        self._replaced_within(first_at, last_at),
                        "session rebinding: a begun portal survives acknowledged durable view replacement",
                        {**details, "marker": marker},
                    )
            finally:
                self._end_read(token)

        return self.pool.call(
            self._session(d),
            Op("rebinding cursor", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        done: list[bool] = []
        parameters = model.Parameters(2, 1, False)
        while not done and time.monotonic() < deadline:
            for i in range(SESSIONS):
                op = self._prepared_op(parameters, done)
                if self.pool.call(i, op, 60.0) != Status.NOT_SENT:
                    break
            if not done:
                time.sleep(1)
        always(
            bool(done),
            "session rebinding: a prepared read of the durable view matches after faults stop",
            {},
        )
