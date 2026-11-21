# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Window functions under retractions: maintained windows equal recomputation.

Writes insert, update, move between partitions, and delete rows of one base
table. Each `Spec` of `SPECS` is maintained twice over it, as an indexed view
and as a materialized view of `window_retractions_model.query`: row numbers,
ranks, `lag` and `lead` with per-row offsets and defaults, and aggregates over
a `ROWS` frame and over the default peer frame.

A check reads the base table and every maintained view in one `SELECT`, so all
rows share one timestamp, recomputes each view from the base rows with
`window_retractions_model.evaluate`, and compares. The expectation follows
from the base rows the same read returned, so no write outcome is needed and a
lost or doubly applied write cannot fail the comparison. The client history
owns durability.

The model orders a partition by `(o, id)`, so ids must be unique. Every insert
takes ids from a counter that never goes back, and a check that still finds an
id twice reports that and compares nothing. The assertion messages are those
of the legacy window retractions driver where it had the same check.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis import window_retractions_model as model
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

SCHEMA = "win"
CLUSTER = "driver"
MAX_ROWS = 36
"""The widest span of ids the table may hold, see `Windows.write`. Window
dataflows recompute a whole partition per change, and the reference is
quadratic in a partition's size."""
SPECS = (
    model.Spec(descending=False, nulls_first=False, lower=-3, upper=-1),
    model.Spec(descending=True, nulls_first=True, lower=1, upper=3),
    model.Spec(descending=False, nulls_first=True, lower=-2, upper=1),
    model.Spec(descending=True, nulls_first=False, lower=-2, upper=1),
)
VARIANTS = ("v", "mv")
"""The indexed view and the materialized view of a spec."""
BASE_COLUMNS = model.INPUT_COLUMNS.split(",")
COLUMNS = model.OUTPUT_COLUMNS.split(",")
GROUPS = (None, 0, 1)
ORDER_KEYS = (None, -2, -1, 0, 1, 2)
"""Few values, so partitions hold peers, and NULL, so its placement matters."""
OFFSETS = (None, -2, -1, 1, 2)
"""Never 0: the model leaves `IGNORE NULLS` with a zero offset undefined."""
WRITE_KINDS = (
    "insert",
    "insert",
    "insert_many",
    "update_value",
    "update_order",
    "update_offset",
    "move",
    "move",
    "delete",
    "delete_group",
)


def view(spec: int, variant: str) -> str:
    return f"w{spec}_{variant}"


def setup_statements() -> list[str]:
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t"
        " (id bigint, g int, o int, v int, off int, def int)",
    ]
    for i, spec in enumerate(SPECS):
        query = model.query(f"{SCHEMA}.t", spec)
        statements += [
            f"CREATE VIEW IF NOT EXISTS {SCHEMA}.{view(i, 'v')} AS {query}",
            f"CREATE INDEX IF NOT EXISTS {view(i, 'v')}_idx IN CLUSTER {CLUSTER}"
            f" ON {SCHEMA}.{view(i, 'v')} (id)",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.{view(i, 'mv')}"
            f" IN CLUSTER {CLUSTER} AS {query}",
        ]
    return statements


def check_query() -> str:
    """The base table and every maintained view as one relation of bigints,
    each row tagged with its origin."""
    padding = ", ".join("NULL::bigint" for _ in COLUMNS[len(BASE_COLUMNS) :])
    base = ", ".join(f"{c}::bigint AS {c}" for c in BASE_COLUMNS)
    parts = [f"SELECT 't' AS tag, {base}, {padding} FROM {SCHEMA}.t"]
    output = ", ".join(f"{c}::bigint" for c in COLUMNS)
    for i in range(len(SPECS)):
        for variant in VARIANTS:
            name = view(i, variant)
            parts.append(f"SELECT '{name}', {output} FROM {SCHEMA}.{name}")
    return f"SELECT q.* FROM ({' UNION ALL '.join(parts)}) AS q"


def _literal(value: int | None) -> str:
    return "NULL" if value is None else str(value)


def _row(key: int, d: Draw) -> str:
    values = (
        key,
        d.choice(GROUPS),
        d.choice(ORDER_KEYS),
        None if d.boolean(0.2) else d.integer(-20, 20),
        d.choice(OFFSETS),
        d.integer(-200, 200),
    )
    return f"({', '.join(_literal(v) for v in values)})"


def _group_predicate(group: int | None) -> str:
    return "g IS NULL" if group is None else f"g = {group}"


def write_statement(kind: str, fresh: list[int], live: list[int], d: Draw) -> str:
    """The statement of one write. `fresh` are ids no row ever had, `live` the
    ids the last check saw, of which at least one exists unless `kind` inserts."""
    t = f"{SCHEMA}.t"
    if kind == "insert":
        return f"INSERT INTO {t} VALUES {_row(fresh[0], d)}"
    if kind == "insert_many":
        return f"INSERT INTO {t} VALUES {', '.join(_row(key, d) for key in fresh)}"
    if kind == "delete_group":
        return f"DELETE FROM {t} WHERE {_group_predicate(d.choice(GROUPS))}"
    key = d.choice(live)
    if kind == "update_value":
        value = _literal(None if d.boolean(0.2) else d.integer(-20, 20))
        return f"UPDATE {t} SET v = {value} WHERE id = {key}"
    if kind == "update_order":
        return f"UPDATE {t} SET o = {_literal(d.choice(ORDER_KEYS))} WHERE id = {key}"
    if kind == "update_offset":
        return (
            f"UPDATE {t} SET off = {_literal(d.choice(OFFSETS))},"
            f" def = {d.integer(-200, 200)} WHERE id = {key}"
        )
    if kind == "move":
        return f"UPDATE {t} SET g = {_literal(d.choice(GROUPS))} WHERE id = {key}"
    assert kind == "delete"
    return f"DELETE FROM {t} WHERE id = {key}"


def _diff(expected: Counter[Any], observed: Counter[Any]) -> dict[str, Any]:
    return {
        "missing": list((expected - observed).items())[:4],
        "unexpected": list((observed - expected).items())[:4],
    }


class Windows:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next_id = 1
        self._trimmed = 0
        """Every id up to this one has been deleted, or is about to be."""
        self.groups: dict[int, int | None] = {}
        """The partition of every row the last check saw, by id."""
        self.serializable_share = 0.0
        self.query = check_query()

    def configure(self, d: Draw) -> None:
        self.serializable_share = d.choice((0.0, 0.3))

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def write(self, d: Draw) -> Status:
        with self._lock:
            live = sorted(self.groups)
            # Ids only grow, so deleting below a bound caps the table whether
            # or not checks run and whichever earlier deletes applied.
            trim = self._next_id - 1 - self._trimmed > MAX_ROWS
            if trim:
                self._trimmed = self._next_id - 1 - MAX_ROWS // 2
            bound = self._trimmed
        if trim:
            kind, statement = "trim", f"DELETE FROM {SCHEMA}.t WHERE id <= {bound}"
        else:
            kind = d.choice(WRITE_KINDS)
            if not live and not kind.startswith("insert"):
                kind = "insert"
            count = d.integer(2, 4) if kind == "insert_many" else 1
            with self._lock:
                fresh = list(range(self._next_id, self._next_id + count))
                if kind.startswith("insert"):
                    self._next_id += count
            statement = write_statement(kind, fresh, live, d)
        linger = d.choice(ABANDON_LINGER_SECONDS) if d.boolean(ABANDON_SHARE) else None

        def execute(conn: psycopg.Connection) -> None:
            if linger is not None:
                abandon(conn, statement.encode(), linger)
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            pass

        return self.ctx.sessions.call(
            self._session(d),
            Op(
                f"windows {kind}",
                execute,
                complete,
                {"cluster": CLUSTER, "transaction_isolation": "'strict serializable'"},
            ),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, isolation: str, compared: list[bool]) -> Op:
        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(self.query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            if self.compare(outcome.value):
                compared.append(True)

        return Op(
            "windows check",
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
            self._session(d), self._read_op(isolation, []), RULE_WAIT_SECONDS
        )

    def compare(self, rows: list[tuple[Any, ...]]) -> bool:
        """Judge one read of `check_query`. Returns whether the views were
        compared."""
        state: dict[str, Counter[tuple[Any, ...]]] = {}
        for tag, *payload in rows:
            state.setdefault(tag, Counter())[tuple(payload)] += 1
        base: list[model.Input] = [
            (row[0], row[1], row[2], row[3], row[4], row[5])
            for row in state.get("t", Counter()).elements()
        ]
        ids = Counter(row[0] for row in base)
        duplicates = sorted(key for key, count in ids.items() if count > 1)
        always(
            not duplicates,
            "window retractions: every base row id appears once",
            {"ids": duplicates[:10]},
        )
        if duplicates:
            return False
        groups = {row[0]: row[1] for row in base}
        with self._lock:
            previous, self.groups = self.groups, groups

        mismatches: dict[str, dict[str, Any]] = {}
        for i, spec in enumerate(SPECS):
            expected = model.evaluate(base, spec)
            for variant in VARIANTS:
                observed = state.get(view(i, variant), Counter())
                if observed != expected:
                    mismatches[view(i, variant)] = {
                        "order": spec.order(unique=True),
                        "frame": spec.frame,
                        **_diff(expected, observed),
                    }

        def bad(variant: str) -> dict[str, Any]:
            return {k: v for k, v in mismatches.items() if k.endswith(f"_{variant}")}

        always(
            not bad("v"),
            "window retractions: maintained index equals the input-derived window oracle",
            {"rows": len(base), "views": bad("v")},
        )
        always(
            not bad("mv"),
            "window retractions: materialized windows equal the input-derived window oracle",
            {"rows": len(base), "views": bad("mv")},
        )

        by_group: dict[int | None, list[model.Input]] = {}
        for row in base:
            by_group.setdefault(row[1], []).append(row)
        peers = any(
            len({row[2] for row in members}) < len(members)
            for members in by_group.values()
        )
        counts = {"rows": len(base), "partitions": len(by_group)}
        sometimes(
            peers and any(row[4] is None for row in base),
            "window retractions: checked peer ranks and nullable offsets against generated input",
            counts,
        )
        # The first row of a partition has no row before it, and the last none
        # after it, so a frame that excludes the current row is empty there.
        sometimes(
            bool(base),
            "window retractions: checked empty ROWS frames and default peer frames",
            counts,
        )
        sometimes(
            any(key in previous and previous[key] != g for key, g in groups.items()),
            "window retractions: a check compared windows after a row moved to another partition",
            counts,
        )
        sometimes(
            any(key not in groups and g in by_group for key, g in previous.items()),
            "window retractions: a check compared windows after a row left a partition that kept others",
            counts,
        )
        return True

    def eventually(self, deadline: float) -> None:
        compared: list[bool] = []
        while not compared and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(
                        i, self._read_op("strict serializable", compared), 60.0
                    )
                    != Status.NOT_SENT
                ):
                    break
            if not compared:
                time.sleep(1)
        always(
            bool(compared),
            "window retractions: every maintained window can be compared with a current reference after faults stop",
            {},
        )
